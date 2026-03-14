import { parseDate, cleanText, getField } from './utils.js';

export function transformBrokerRow(row: Record<string, any>): Record<string, any> {
  return {
    broker_name: cleanText(getField(row, 'Broker Name', 'broker_name', 'Name')),
    broker_name_ar: cleanText(getField(row, 'Broker Name (AR)', 'broker_name_ar', 'Name AR')),
    license_number: cleanText(getField(row, 'License Number', 'license_number')),
    license_type: cleanText(getField(row, 'License Type', 'license_type')),
    license_expiry: parseDate(getField(row, 'License Expiry', 'license_expiry', 'Expiry Date')),
    company: cleanText(getField(row, 'Company', 'company')),
    status: cleanText(getField(row, 'Status', 'status')),
    raw_data: row,
  };
}
