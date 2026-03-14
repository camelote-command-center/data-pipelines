import { parseDate, parseNumeric, cleanText, getField } from './utils.js';

export function transformValuationRow(row: Record<string, any>): Record<string, any> {
  return {
    valuation_date: parseDate(getField(row, 'Valuation Date', 'valuation_date')),
    area: cleanText(getField(row, 'Area', 'area')),
    property_type: cleanText(getField(row, 'Property Type', 'property_type')),
    property_sub_type: cleanText(getField(row, 'Property Sub Type', 'property_sub_type')),
    usage: cleanText(getField(row, 'Usage', 'usage')),
    valuation_amount: parseNumeric(getField(row, 'Valuation Amount', 'valuation_amount', 'Amount')),
    property_size_sqm: parseNumeric(getField(row, 'Property Size (sq.m)', 'Property Size', 'property_size_sqm')),
    master_project: cleanText(getField(row, 'Master Project', 'master_project')),
    project: cleanText(getField(row, 'Project', 'project')),
    raw_data: row,
  };
}
