import { parseNumeric, parseInt_, parseBool, cleanText, getField } from './utils.js';

export function transformBuildingRow(row: Record<string, any>): Record<string, any> {
  return {
    building_name: cleanText(getField(row, 'Building Name', 'building_name')),
    area: cleanText(getField(row, 'Area', 'area')),
    building_type: cleanText(getField(row, 'Building Type', 'building_type', 'Type')),
    floors: parseInt_(getField(row, 'Floors', 'floors', 'Number of Floors')),
    units_count: parseInt_(getField(row, 'Units Count', 'units_count', 'Number of Units')),
    is_freehold: parseBool(getField(row, 'Is Free Hold?', 'Is Free Hold', 'is_freehold')),
    usage: cleanText(getField(row, 'Usage', 'usage')),
    master_project: cleanText(getField(row, 'Master Project', 'master_project')),
    project: cleanText(getField(row, 'Project', 'project')),
    nearest_metro: cleanText(getField(row, 'Nearest Metro', 'nearest_metro')),
    nearest_mall: cleanText(getField(row, 'Nearest Mall', 'nearest_mall')),
    nearest_landmark: cleanText(getField(row, 'Nearest Landmark', 'nearest_landmark')),
    raw_data: row,
  };
}
