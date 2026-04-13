/**
 * Field mapper — maps Excel column headers to canonical DB fields.
 * Only these 9 fields are supported. All other columns are ignored.
 */

export const FIELD_ALIASES: Record<string, string[]> = {
  cdr_number:        ['cdrno', 'cdr no', 'cdr number', 'a party', 'a-party', 'calling number', 'msisdn'],
  b_party:           ['b party', 'b-party', 'called number', 'dialled number', 'bnumber'],
  b_party_internal:  ['b party internal', 'b_party_internal', 'b party int'],
  name_b_party:      ['name b party', 'name b_party', 'name', 'subscriber name', 'customer name'],
  father_name:       ['father b party', 'father b_party', 'father name', "father's name", 'f/name'],
  permanent_address: ['permanent address b party', 'permanent address b_party', 'address', 'permanent address', 'subscriber address'],
  call_date:         ['date', 'call date', 'date of call', 'cdr date'],
  main_city:         ['main city(first cellid)', 'main city (first cellid)', 'main city', 'city', 'location city'],
  sub_city:          ['sub city (first cellid)', 'sub city(first cellid)', 'sub city', 'sub-city', 'district'],
};

function normalize(str: string): string {
  return str.toLowerCase().replace(/[\s_\-\.]+/g, ' ').trim();
}

export function buildColumnMapping(headers: string[]): Record<string, string> {
  const mapping: Record<string, string> = {};
  const usedCanonical = new Set<string>();

  for (const header of headers) {
    const normalizedHeader = normalize(header);
    let matched = false;

    for (const [canonical, aliases] of Object.entries(FIELD_ALIASES)) {
      if (usedCanonical.has(canonical)) continue;
      if (aliases.some(alias => normalize(alias) === normalizedHeader)) {
        mapping[header] = canonical;
        usedCanonical.add(canonical);
        matched = true;
        break;
      }
    }

    if (!matched) mapping[header] = '__ignore__';
  }

  return mapping;
}

export function mapRow(
  rawRow: Record<string, unknown>,
  columnMapping: Record<string, string>
): Record<string, unknown> {
  const mapped: Record<string, unknown> = {};
  for (const [header, value] of Object.entries(rawRow)) {
    const canonical = columnMapping[header];
    if (canonical && canonical !== '__ignore__') {
      mapped[canonical] = value;
    }
  }
  return mapped;
}
