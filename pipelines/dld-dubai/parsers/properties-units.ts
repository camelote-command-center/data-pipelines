import { parseNumeric, parseBool, cleanText, getField } from './utils.js';

export function transformUnitRow(row: Record<string, any>): Record<string, any> {
  return {
    unit_number: cleanText(getField(row, 'Unit Number', 'unit_number')),
    building_name: cleanText(getField(row, 'Building Name', 'building_name')),
    area: cleanText(getField(row, 'Area', 'area')),
    unit_type: cleanText(getField(row, 'Unit Type', 'unit_type', 'Property Type')),
    unit_sub_type: cleanText(getField(row, 'Unit Sub Type', 'unit_sub_type', 'Property Sub Type')),
    unit_size_sqm: parseNumeric(getField(row, 'Unit Size (sq.m)', 'Unit Size', 'unit_size_sqm', 'Property Size (sq.m)')),
    rooms: cleanText(getField(row, 'Room(s)', 'Rooms', 'rooms')),
    parking: cleanText(getField(row, 'Parking', 'parking')),
    floor: cleanText(getField(row, 'Floor', 'floor')),
    is_freehold: parseBool(getField(row, 'Is Free Hold?', 'Is Free Hold', 'is_freehold')),
    usage: cleanText(getField(row, 'Usage', 'usage')),
    master_project: cleanText(getField(row, 'Master Project', 'master_project')),
    project: cleanText(getField(row, 'Project', 'project')),
    raw_data: row,
  };
}
