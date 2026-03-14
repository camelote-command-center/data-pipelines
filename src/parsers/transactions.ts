import { parseDate, parseBool, parseNumeric, parseInt_, cleanText, getField } from './utils.js';

function parseFreehold(value: any): boolean | null {
  if (value === null || value === undefined || value === '') return null;
  const str = String(value).trim().toLowerCase();
  if (str === 'free hold' || str === 'freehold' || str === 'yes' || str === 'true') return true;
  if (str === 'no' || str === 'false') return false;
  return null;
}

function parseOffplan(value: any): boolean | null {
  if (value === null || value === undefined || value === '') return null;
  const str = String(value).trim().toLowerCase();
  if (str === 'off-plan' || str === 'offplan' || str === 'yes' || str === 'true') return true;
  if (str === 'ready' || str === 'no' || str === 'false') return false;
  return null;
}

export function transformTransactionRow(row: Record<string, any>): Record<string, any> {
  return {
    transaction_number: cleanText(getField(row, 'TRANSACTION_NUMBER', 'Transaction Number', 'transaction_number')),
    transaction_date: parseDate(getField(row, 'INSTANCE_DATE', 'Transaction Date', 'transaction_date')),
    transaction_type: cleanText(getField(row, 'GROUP_EN', 'Transaction Type', 'transaction_type')),
    transaction_sub_type: cleanText(getField(row, 'PROCEDURE_EN', 'Transaction sub type', 'Transaction Sub Type', 'transaction_sub_type')),
    registration_type: cleanText(getField(row, 'Registration type', 'Registration Type', 'registration_type')),
    is_freehold: parseFreehold(getField(row, 'IS_FREE_HOLD_EN', 'Is Free Hold?', 'Is Free Hold', 'is_freehold', 'is_free_hold')),
    is_offplan: parseOffplan(getField(row, 'IS_OFFPLAN_EN', 'Is Offplan', 'is_offplan')),
    usage: cleanText(getField(row, 'USAGE_EN', 'Usage', 'usage')),
    area: cleanText(getField(row, 'AREA_EN', 'Area', 'area')),
    property_type: cleanText(getField(row, 'PROP_TYPE_EN', 'Property Type', 'property_type')),
    property_sub_type: cleanText(getField(row, 'PROP_SB_TYPE_EN', 'Property Sub Type', 'property_sub_type')),
    amount: parseNumeric(getField(row, 'TRANS_VALUE', 'Amount', 'amount')),
    transaction_size_sqm: parseNumeric(getField(row, 'PROCEDURE_AREA', 'Transaction Size (sq.m)', 'Transaction Size', 'transaction_size_sqm')),
    property_size_sqm: parseNumeric(getField(row, 'ACTUAL_AREA', 'Property Size (sq.m)', 'Property Size', 'property_size_sqm')),
    rooms: cleanText(getField(row, 'ROOMS_EN', 'Room(s)', 'Rooms', 'rooms')),
    parking: cleanText(getField(row, 'PARKING', 'Parking', 'parking')),
    nearest_metro: cleanText(getField(row, 'NEAREST_METRO_EN', 'Nearest Metro', 'nearest_metro')),
    nearest_mall: cleanText(getField(row, 'NEAREST_MALL_EN', 'Nearest Mall', 'nearest_mall')),
    nearest_landmark: cleanText(getField(row, 'NEAREST_LANDMARK_EN', 'Nearest Landmark', 'nearest_landmark')),
    buyer_count: parseInt_(getField(row, 'TOTAL_BUYER', 'No. of Buyer', 'No of Buyer', 'buyer_count')),
    seller_count: parseInt_(getField(row, 'TOTAL_SELLER', 'No. of Seller', 'No of Seller', 'seller_count')),
    master_project: cleanText(getField(row, 'MASTER_PROJECT_EN', 'Master Project', 'master_project')),
    project: cleanText(getField(row, 'PROJECT_EN', 'Project', 'project')),
    raw_data: row,
  };
}
