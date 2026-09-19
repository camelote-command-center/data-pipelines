"""
Minimal glTF 2.0 writer for city tiles, with the three extensions trimesh cannot emit:

  EXT_mesh_features + EXT_structural_metadata   every building vertex carries _FEATURE_ID_0 -> a property
                                                 table row holding the building's EGID, so Cesium picking
                                                 (feature.getProperty('egid')) knows which building was hit
  EXT_mesh_gpu_instancing                        each tree variant is ONE small mesh drawn N times with
                                                 per-instance TRANSLATION / SCALE / ROTATION — measured
                                                 need: 8,315 trees as merged geometry cost ~13 M triangles
                                                 and 531 MB of GPU memory for Geneve-Cite

Geometry is non-indexed (one vertex per triangle corner) so normals are per face: flat shading keeps roof
ridges and wall edges crisp. Textures are embedded JPEGs.
"""
import io
import numpy as np
import pygltflib as G

FLOAT, UINT = 5126, 5125
ARRAY_BUFFER = 34962


class Builder:
    def __init__(self):
        self.g = G.GLTF2(asset=G.Asset(version="2.0", generator="lamap ge_city3d"))
        self.g.scenes = [G.Scene(nodes=[])]
        self.g.scene = 0
        self.blob = bytearray()
        self.materials = {}

    def _view(self, data: bytes, target=None):
        while len(self.blob) % 4:
            self.blob.append(0)
        off = len(self.blob)
        self.blob += data
        self.g.bufferViews.append(G.BufferView(buffer=0, byteOffset=off, byteLength=len(data), target=target))
        return len(self.g.bufferViews) - 1

    def _acc(self, arr, type_, ctype=FLOAT, minmax=False, target=ARRAY_BUFFER):
        arr = np.ascontiguousarray(arr, dtype=np.float32 if ctype == FLOAT else np.uint32)
        v = self._view(arr.tobytes(), target)
        a = G.Accessor(bufferView=v, componentType=ctype, count=len(arr), type=type_)
        if minmax:
            a.min = arr.min(axis=0).tolist(); a.max = arr.max(axis=0).tolist()
        self.g.accessors.append(a)
        return len(self.g.accessors) - 1

    def material(self, key, image=None, color=(1, 1, 1, 1), rough=0.9):
        if key in self.materials:
            return self.materials[key]
        pbr = G.PbrMetallicRoughness(metallicFactor=0.0, roughnessFactor=rough, baseColorFactor=list(color))
        if image is not None:
            b = io.BytesIO(); image.convert("RGB").save(b, "JPEG", quality=84)
            iv = self._view(b.getvalue())
            self.g.images.append(G.Image(bufferView=iv, mimeType="image/jpeg"))
            if not self.g.samplers:
                self.g.samplers.append(G.Sampler(magFilter=9729, minFilter=9987, wrapS=10497, wrapT=10497))
            self.g.textures.append(G.Texture(sampler=0, source=len(self.g.images) - 1))
            pbr.baseColorTexture = G.TextureInfo(index=len(self.g.textures) - 1)
        self.g.materials.append(G.Material(name=key, pbrMetallicRoughness=pbr, doubleSided=True))
        self.materials[key] = len(self.g.materials) - 1
        return self.materials[key]

    @staticmethod
    def flat_normals(P):
        t = P.reshape(-1, 3, 3)
        n = np.cross(t[:, 1] - t[:, 0], t[:, 2] - t[:, 0])
        n /= np.maximum(np.linalg.norm(n, axis=1, keepdims=True), 1e-12)
        return np.repeat(n, 3, axis=0)

    def building_mesh(self, prims, egids, lit=False):
        """prims: list of (material_index, positions Nx3 (N % 3 == 0), uvs Nx2 | None, feature_ids N[, tint Nx4 0..1]).
        A per-vertex tint (COLOR_0) multiplies a NEUTRAL facade texture, so one texture serves every palette of a
        style: ~10 materials per tile instead of ~45, i.e. 3-4x fewer draw calls (measured: 1,686 for Geneve-Cite).

        lit=False (default) writes NO normals, so CesiumJS renders the buildings UNLIT: textures at full
        brightness, cast shadows still drawn. That is the look the operator approved (v1 had no normals by
        accident). With normals, real sun shading turns every facade facing away from the sun dark."""
        gprims = []
        for pr in prims:
            mat, P, UV, FID = pr[:4]
            attrs = G.Attributes(POSITION=self._acc(P, "VEC3", minmax=True))
            if len(pr) > 4 and pr[4] is not None:
                attrs.COLOR_0 = self._acc(pr[4], "VEC4")
            if lit:
                attrs.NORMAL = self._acc(self.flat_normals(P), "VEC3")
            if UV is not None:
                attrs.TEXCOORD_0 = self._acc(UV, "VEC2")
            fid = self._acc(np.asarray(FID, dtype=np.float32), "SCALAR")
            setattr(attrs, "_FEATURE_ID_0", fid)
            gprims.append(G.Primitive(attributes=attrs, material=mat, extensions={
                "EXT_mesh_features": {"featureIds": [{"featureCount": len(egids), "attribute": 0, "propertyTable": 0}]}}))
        self.g.meshes.append(G.Mesh(name="buildings", primitives=gprims))
        self.g.nodes.append(G.Node(name="buildings", mesh=len(self.g.meshes) - 1))
        self.g.scenes[0].nodes.append(len(self.g.nodes) - 1)
        # property table: one row per feature id, column egid (UINT32 fits every Swiss EGID)
        pv = self._view(np.asarray(egids, dtype=np.uint32).tobytes())
        self.g.extensions = self.g.extensions or {}
        self.g.extensions["EXT_structural_metadata"] = {
            "schema": {"id": "lamap_city", "classes": {"building": {"properties": {"egid": {"type": "SCALAR", "componentType": "UINT32"}}}}},
            "propertyTables": [{"class": "building", "count": len(egids), "properties": {"egid": {"values": pv}}}]}
        for e in ("EXT_mesh_features", "EXT_structural_metadata"):
            if e not in self.g.extensionsUsed:
                self.g.extensionsUsed.append(e)

    def instanced(self, name, P, C, translations, scales, rotations=None):
        """One mesh (non-indexed P with per-vertex colours C in 0..1 RGBA) drawn once per instance."""
        if len(translations) == 0:
            return
        attrs = G.Attributes(POSITION=self._acc(P, "VEC3", minmax=True), NORMAL=self._acc(self.flat_normals(P), "VEC3"),
                             COLOR_0=self._acc(C, "VEC4"))
        mat = self.material("tree_vertex_colour", color=(1, 1, 1, 1), rough=1.0)
        self.g.meshes.append(G.Mesh(name=name, primitives=[G.Primitive(attributes=attrs, material=mat)]))
        inst = {"TRANSLATION": self._acc(translations, "VEC3", target=None), "SCALE": self._acc(scales, "VEC3", target=None)}
        if rotations is not None:
            inst["ROTATION"] = self._acc(rotations, "VEC4", target=None)
        self.g.nodes.append(G.Node(name=name, mesh=len(self.g.meshes) - 1, extensions={"EXT_mesh_gpu_instancing": {"attributes": inst}}))
        self.g.scenes[0].nodes.append(len(self.g.nodes) - 1)
        for e in ("EXT_mesh_gpu_instancing",):
            if e not in self.g.extensionsUsed:
                self.g.extensionsUsed.append(e)
                self.g.extensionsRequired = list(set((self.g.extensionsRequired or []) + [e]))

    def save(self, path):
        while len(self.blob) % 4:
            self.blob.append(0)
        self.g.buffers = [G.Buffer(byteLength=len(self.blob))]
        self.g.set_binary_blob(bytes(self.blob))
        self.g.save_binary(path)
