import React from 'react';

import IconButton from './IconButton';

const CheckBox = ({
  checked = false,
  label,
  onChange,
  size = 24,
  ...otherProps
}) => (
  <IconButton
    name={checked ? 'check-box' : 'unchecked-box'}
    size={size}
    label={label}
    onClick={onChange && (() => onChange(!checked))}
    {...otherProps}
  />
);
CheckBox.displayName = 'CheckBox';

export default CheckBox;
