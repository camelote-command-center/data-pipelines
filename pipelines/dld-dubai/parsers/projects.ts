import { parseDate, parseBool, parseNumeric, parseInt_, cleanText, getField } from './utils.js';

export function transformProjectRow(row: Record<string, any>): Record<string, any> {
  return {
    project_name: cleanText(getField(row, 'Project Name', 'project_name')),
    master_project: cleanText(getField(row, 'Master Project', 'master_project')),
    developer: cleanText(getField(row, 'Developer', 'developer')),
    area: cleanText(getField(row, 'Area', 'area')),
    status: cleanText(getField(row, 'Status', 'status')),
    start_date: parseDate(getField(row, 'Start Date', 'start_date')),
    completion_date: parseDate(getField(row, 'Completion Date', 'completion_date', 'Expected Completion')),
    percentage_completed: parseNumeric(getField(row, 'Percentage Completed', 'percentage_completed', 'Completion Percentage')),
    is_freehold: parseBool(getField(row, 'Is Free Hold?', 'Is Free Hold', 'is_freehold')),
    usage: cleanText(getField(row, 'Usage', 'usage')),
    raw_data: row,
  };
}
