/**
 * Normalizes phone numbers to E.164 format.
 * Assumes Indian numbers by default (country code +91).
 */

export function normalizePhone(raw: string | null | undefined, defaultCountryCode = '91'): string | null {
  if (!raw) return null;

  // Remove all non-digit characters except leading +
  let cleaned = String(raw).trim().replace(/[^\d+]/g, '');

  if (!cleaned) return null;

  // Already has + prefix
  if (cleaned.startsWith('+')) {
    const digits = cleaned.slice(1);
    if (digits.length >= 10 && digits.length <= 15) {
      return `+${digits}`;
    }
    return null;
  }

  // Remove leading zeros
  cleaned = cleaned.replace(/^0+/, '');

  // 10-digit Indian mobile number
  if (cleaned.length === 10) {
    return `+${defaultCountryCode}${cleaned}`;
  }

  // 12-digit with country code (e.g., 919876543210)
  if (cleaned.length === 12 && cleaned.startsWith('91')) {
    return `+${cleaned}`;
  }

  // 11-digit with leading 0 already stripped
  if (cleaned.length >= 10 && cleaned.length <= 15) {
    return `+${defaultCountryCode}${cleaned.slice(-10)}`;
  }

  return cleaned.length > 0 ? cleaned : null;
}
