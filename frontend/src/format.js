export function percent(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "n/a";
  return `${(Number(value) * 100).toFixed(digits)}%`;
}

export function number(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "n/a";
  return Number(value).toLocaleString(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  });
}

export function integer(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "n/a";
  return Number(value).toLocaleString();
}

export function compactDate(value) {
  if (!value) return "n/a";
  return value.slice(0, 10);
}

export function shortTitle(value, limit = 110) {
  if (!value) return "(untitled)";
  return value.length > limit ? `${value.slice(0, limit - 1)}...` : value;
}

export const signalLabels = {
  pub_deviation: "Deviation",
  pub_zscore: "52-week z-score",
  pub_4w_dev: "4-week deviation",
};
