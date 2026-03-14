import { parseDate, parseBool, parseNumeric, parseInt_, cleanText, getField } from './utils.js';

export function transformRentalRow(row: Record<string, any>): Record<string, any> {
  return {
    registration_date: parseDate(getField(row, 'Registration Date', 'registration_date')),
    start_date: parseDate(getField(row, 'Start Date', 'start_date')),
    end_date: parseDate(getField(row, 'End Date', 'end_date')),
    version: cleanText(getField(row, 'Version', 'version')),
    area: cleanText(getField(row, 'Area', 'area')),
    contract_amount: parseNumeric(getField(row, 'Contract Amount', 'contract_amount')),
    annual_amount: parseNumeric(getField(row, 'Annual Amount', 'annual_amount')),
    is_freehold: parseBool(getField(row, 'Is Free Hold?', 'Is Free Hold', 'is_freehold', 'is_free_hold')),
    property_size_sqm: parseNumeric(getField(row, 'Property Size (sq.m)', 'Property Size', 'property_size_sqm')),
    property_type: cleanText(getField(row, 'Property Type', 'property_type')),
    property_sub_type: cleanText(getField(row, 'Property Sub Type', 'property_sub_type')),
    rooms: cleanText(getField(row, 'Number of Rooms', 'Rooms', 'rooms')),
    usage: cleanText(getField(row, 'Usage', 'usage')),
    nearest_metro: cleanText(getField(row, 'Nearest Metro', 'nearest_metro')),
    nearest_mall: cleanText(getField(row, 'Nearest Mall', 'nearest_mall')),
    nearest_landmark: cleanText(getField(row, 'Nearest Landmark', 'nearest_landmark')),
    parking: cleanText(getField(row, 'Parking', 'parking')),
    unit_count: parseInt_(getField(row, 'No of Units', 'No. of Units', 'unit_count')),
    master_project: cleanText(getField(row, 'Master Project', 'master_project')),
    project: cleanText(getField(row, 'Project', 'project')),
    raw_data: row,
  };
}
