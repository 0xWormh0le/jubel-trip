import fp from 'lodash/fp';

export const parseQueryString = (string) =>
  fp.compose(
    JSON.parse,
    JSON.stringify,
    fp.reduce((acc, part) => {
      const [name, value] = part.split('=');
      acc[name] = decodeURIComponent(value || '');
      return acc;
    }, {}),
    (str) => (str ? str.split('&') : []),
    fp.replace('?', '')
  )(string);

export const jsonToQueryString = (obj) => {
  const pairs = [];
  obj &&
    Object.keys(obj).forEach((key) => {
      if (obj[key]) {
        const value = encodeURIComponent(obj[key]);
        value && pairs.push(`${key}=${value}`);
      }
    });

  return pairs.length ? `?${pairs.join('&')}` : '';
};

export const updateQueryStringParameter = (uri, key, value) => {
  const re = new RegExp(`([?&])${key}=.*?(&|$)`, 'i');
  const separator = uri.indexOf('?') !== -1 ? '&' : '?';
  return uri.match(re)
    ? uri.replace(re, `$1${key}=${value}$2`)
    : `${uri + separator + key}=${value}`;
};
