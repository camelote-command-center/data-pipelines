import { cleanText, getField } from './utils.js';

export function transformDeveloperRow(row: Record<string, any>): Record<string, any> {
  return {
    developer_name: cleanText(getField(row, 'Developer Name', 'developer_name', 'Name')),
    developer_name_ar: cleanText(getField(row, 'Developer Name (AR)', 'developer_name_ar', 'Name AR')),
    license_number: cleanText(getField(row, 'License Number', 'license_number')),
    status: cleanText(getField(row, 'Status', 'status')),
    raw_data: row,
  };
}
