const targetedProps = (props, prefix, defaults = {}) => {
  const obj = { ...defaults };
  const plen = prefix.length;
  const pfxPattern = new RegExp(`^${prefix}`);
  Object.keys(props).forEach((key) => {
    if (key.startsWith(prefix) && plen < key.length) {
      const newKey = key.replace(pfxPattern, '');
      obj[newKey.substring(0, 1).toLowerCase() + newKey.substring(1)] =
        props[key];
      // eslint-disable-next-line no-param-reassign
      delete props[key];
    }
  });
  return obj;
};

export default targetedProps;
