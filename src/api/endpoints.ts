export interface DatasetEndpoint {
  slug: string;
  table: string;
  dateField: string | null;
  description: string;
}

export const ENDPOINTS: Record<string, DatasetEndpoint> = {
  transactions: {
    slug: 'dld_transactions-open-api',
    table: 'bronze_ae.transactions',
    dateField: 'Transaction Date',
    description: 'DLD sales, mortgages, gifts',
  },
  rentals: {
    slug: 'dld_rentals-open-api',
    table: 'bronze_ae.rentals',
    dateField: 'Registration Date',
    description: 'Ejari rental contracts',
  },
  projects: {
    slug: 'dld_projects-open-api',
    table: 'bronze_ae.projects',
    dateField: null,
    description: 'Development projects',
  },
  valuations: {
    slug: 'dld_valuations-open-api',
    table: 'bronze_ae.valuations',
    dateField: null,
    description: 'Property valuations',
  },
  land: {
    slug: 'dld_land-open-api',
    table: 'bronze_ae.properties_land',
    dateField: null,
    description: 'Land parcels',
  },
  buildings: {
    slug: 'dld_buildings-open-api',
    table: 'bronze_ae.properties_buildings',
    dateField: null,
    description: 'Buildings',
  },
  units: {
    slug: 'dld_units-open-api',
    table: 'bronze_ae.properties_units',
    dateField: null,
    description: 'Property units',
  },
  brokers: {
    slug: 'dld_brokers-open-api',
    table: 'bronze_ae.brokers',
    dateField: null,
    description: 'Licensed brokers',
  },
  developers: {
    slug: 'dld_developers-open-api',
    table: 'bronze_ae.developers',
    dateField: null,
    description: 'Registered developers',
  },
  // Lookup/reference tables
  sale_index: {
    slug: 'dld_residential_sale_index-open',
    table: 'ref.sale_index',
    dateField: null,
    description: 'Residential sale index',
  },
  transaction_procedures: {
    slug: 'dld_lkp_transaction_procedures-open',
    table: 'ref.transaction_procedures',
    dateField: null,
    description: 'Lookup: transaction procedures',
  },
  transaction_groups: {
    slug: 'dld_lkp_transaction_groups-open',
    table: 'ref.transaction_groups',
    dateField: null,
    description: 'Lookup: transaction groups',
  },
};
