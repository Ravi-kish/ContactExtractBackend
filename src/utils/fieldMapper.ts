/**
 * Fuzzy field mapper: maps raw CSV/Excel column headers to canonical CDR schema fields.
 */

export const FIELD_ALIASES: Record<string, string[]> = {
  cdr_number: ['cdr number', 'a party', 'a-party', 'calling number', 'msisdn', 'a_party', 'anumber', 'caller', 'cdrno', 'cdr no', 'cdrnumber'],
  b_party: ['b party', 'b-party', 'called number', 'dialled number', 'b_party', 'bnumber', 'callee', 'destination'],
  name_b_party: ['name', 'subscriber name', 'name b party', 'customer name', 'name_b_party', 'subscriber', 'name b_party'],
  father_name: ["father name", "father's name", 'f/name', 'father_name', 'fathername', 'father b party', 'father b_party'],
  permanent_address: ['address', 'permanent address', 'subscriber address', 'permanent_address', 'full address', 'permanent address b party', 'permanent address b_party'],
  call_date: ['call date', 'date', 'date of call', 'cdr date', 'call_date', 'calldate'],
  call_time: ['call time', 'time', 'time of call', 'cdr time', 'call_time', 'calltime'],
  duration_seconds: ['duration', 'call duration', 'duration (sec)', 'seconds', 'duration_seconds', 'dur', 'duration(s)'],
  call_type: ['call type', 'type', 'direction', 'moc/mtc', 'call_type', 'calltype', 'service type'],
  first_cell_id: ['first cell id', 'cell id', 'tower id', 'bts id', 'first_cell_id', 'cellid', 'cell_id'],
  first_cell_address: ['first cell address', 'cell address', 'tower address', 'bts address', 'first_cell_address', 'first cell id address'],
  last_cell_id: ['last cell id', 'last_cell_id', 'end cell id', 'last tower id'],
  last_cell_address: ['last cell address', 'last_cell_address', 'end cell address', 'last cell id address'],
  imei: ['imei', 'device imei', 'handset imei', 'equipment id'],
  imsi: ['imsi', 'sim imsi', 'subscriber identity'],
  roaming: ['roaming', 'is roaming', 'roaming flag'],
  circle: ['circle', 'state', 'telecom circle', 'telecom_circle'],
  operator: ['operator', 'service provider', 'carrier', 'network operator'],
  main_city: ['city', 'main city', 'location city', 'main_city', 'main city(first cellid)', 'main city (first cellid)'],
  sub_city: ['sub city', 'sub-city', 'district', 'sub_city', 'subcity', 'sub city (first cellid)'],
  latitude: ['latitude', 'lat', 'gps lat'],
  longitude: ['longitude', 'lon', 'lng', 'long', 'gps lon'],
  device_type: ['device type', 'device_type', 'handset type', 'terminal type'],
  device_manufacturer: ['device manufacturer', 'device_manufacturer', 'handset manufacturer', 'make', 'imei manufacturer'],
  cdr_name: ['cdr name', 'cdr_name', 'file subscriber name', 'name cdrno', 'name cdrnumber'],
  cdr_address: ['cdr address', 'cdr_address', 'file address', 'permanent address cdrno', 'permanent address cdrnumber'],
};

function normalize(str: string): string {
  return str.toLowerCase().replace(/[\s_\-\.]+/g, ' ').trim();
}

export function buildColumnMapping(headers: string[]): Record<string, string> {
  const mapping: Record<string, string> = {};
  const usedCanonical = new Set<string>();

  for (const header of headers) {
    const normalizedHeader = normalize(header);

    for (const [canonical, aliases] of Object.entries(FIELD_ALIASES)) {
      if (usedCanonical.has(canonical)) continue;

      const matched = aliases.some((alias) => normalize(alias) === normalizedHeader);
      if (matched) {
        mapping[header] = canonical;
        usedCanonical.add(canonical);
        break;
      }
    }

    // If not matched, keep as unmapped (will go to raw_row_json)
    if (!mapping[header]) {
      mapping[header] = `__unmapped__${header}`;
    }
  }

  return mapping;
}

export function mapRow(
  rawRow: Record<string, unknown>,
  columnMapping: Record<string, string>
): { mapped: Record<string, unknown>; unmapped: Record<string, unknown> } {
  const mapped: Record<string, unknown> = {};
  const unmapped: Record<string, unknown> = {};

  for (const [header, value] of Object.entries(rawRow)) {
    const canonical = columnMapping[header];
    if (canonical && !canonical.startsWith('__unmapped__')) {
      mapped[canonical] = value;
    } else {
      unmapped[header] = value;
    }
  }

  return { mapped, unmapped };
}
