/**
 * Parses various date/time formats from CDR files into ISO-8601 UTC.
 */

const MONTH_MAP: Record<string, number> = {
  jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5,
  jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11,
};

export function parseDate(raw: string | null | undefined): Date | null {
  if (!raw) return null;
  const str = String(raw).trim();

  // DD/Mon/YYYY or DD-Mon-YYYY e.g. 01/Aug/2022
  const namedMonth = str.match(/^(\d{1,2})[\/\-]([A-Za-z]{3,9})[\/\-](\d{4})$/);
  if (namedMonth) {
    const day = parseInt(namedMonth[1]);
    const month = MONTH_MAP[namedMonth[2].toLowerCase().slice(0, 3)];
    const year = parseInt(namedMonth[3]);
    if (month !== undefined) {
      const d = new Date(Date.UTC(year, month, day));
      if (!isNaN(d.getTime())) return d;
    }
  }

  // Try native Date parse first
  const native = new Date(str);
  if (!isNaN(native.getTime())) return native;

  // DD/MM/YYYY or DD-MM-YYYY or DD.MM.YYYY
  const dmy = str.match(/^(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{4})$/);
  if (dmy) {
    const d = new Date(Date.UTC(parseInt(dmy[3]), parseInt(dmy[2]) - 1, parseInt(dmy[1])));
    if (!isNaN(d.getTime())) return d;
  }

  // YYYY-MM-DD
  const ymd = str.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (ymd) {
    const d = new Date(Date.UTC(parseInt(ymd[1]), parseInt(ymd[2]) - 1, parseInt(ymd[3])));
    if (!isNaN(d.getTime())) return d;
  }

  return null;
}

export function parseTime(raw: string | null | undefined): string | null {
  if (!raw) return null;
  const str = String(raw).trim();

  // HH:MM:SS or HH:MM
  const match = str.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?/);
  if (match) {
    const h = match[1].padStart(2, '0');
    const m = match[2];
    const s = match[3] || '00';
    return `${h}:${m}:${s}`;
  }

  return null;
}

export function combineDatetime(date: Date | null, timeStr: string | null): Date | null {
  if (!date) return null;
  if (!timeStr) return date;

  const [h, m, s] = timeStr.split(':').map(Number);
  const combined = new Date(date);
  combined.setUTCHours(h, m, s || 0, 0);
  return combined;
}

export function parseDuration(raw: string | number | null | undefined): number | null {
  if (raw === null || raw === undefined || raw === '') return null;
  const num = typeof raw === 'number' ? raw : parseFloat(String(raw).replace(/[^\d.]/g, ''));
  return isNaN(num) ? null : Math.round(num);
}
