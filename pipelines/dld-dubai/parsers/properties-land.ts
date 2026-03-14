import { parseNumeric, parseBool, cleanText, getField } from './utils.js';

export function transformLandRow(row: Record<string, any>): Record<string, any> {
  return {
    land_number: cleanText(getField(row, 'Land Number', 'land_number')),
    area: cleanText(getField(row, 'Area', 'area')),
    land_type: cleanText(getField(row, 'Land Type', 'land_type', 'Type')),
    land_size_sqm: parseNumeric(getField(row, 'Land Size (sq.m)', 'Land Size', 'land_size_sqm')),
    is_freehold: parseBool(getField(row, 'Is Free Hold?', 'Is Free Hold', 'is_freehold')),
    usage: cleanText(getField(row, 'Usage', 'usage')),
    master_project: cleanText(getField(row, 'Master Project', 'master_project')),
    project: cleanText(getField(row, 'Project', 'project')),
    raw_data: row,
  };
}
