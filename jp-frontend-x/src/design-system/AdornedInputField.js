import { PropTypes } from 'prop-types';
import React from 'react';

import system from '../utils/System';
import Flex from './Flex';
import IconButton from './IconButton';
import Input from './Input';
import Label from './Label';

const StyledAdornedInputField = system('StyledAdornedInputField', {
  extend: Flex,
  alignItems: 'center',
  boxShadow: 2,
});

const targetedProps = (props, prefix) => {
  const obj = {};
  const plen = prefix.length;
  const pfxPattern = new RegExp(`^${prefix}`);
  Object.keys(props).forEach((key) => {
    if (key.startsWith(prefix) && plen < key.length) {
      const newKey = key.replace(pfxPattern, '');
      obj[newKey.substring(0, 1).toLowerCase() + newKey.substring(1)] =
        props[key];
      delete props[key];
    }
  });
  return obj;
};

const AdornedInputField = ({
  icon,
  iconPlacement = 'right',
  label,
  labelPlacement = 'left',
  p = 2,
  borderRadius = 'sm',
  onChange,
  value,
  type = 'text',
  ...props
}) => {
  const iconProps = targetedProps(props, 'icon');
  const inputProps = targetedProps(props, 'input');
  const labelProps = targetedProps(props, 'label');
  const wantsBr = ['xs', 'sm', 'md', 'lg'].includes(borderRadius);
  const pl = wantsBr ? (x) => (iconPlacement === x ? 'l' : 'r') : () => '';
  if (icon) {
    iconProps.borderRadius = `${borderRadius}${pl('left')}`;
  }
  if (label) {
    labelProps.borderRadius = `${borderRadius}${pl('right')}`;
  }
  if (icon && label) {
    inputProps.borderRadius = 'none';
  } else if (icon) {
    inputProps.borderRadius = `${borderRadius}${pl('right')}`;
  } else {
    inputProps.borderRadius = `${borderRadius}${pl('right')}`;
  }
  return (
    <StyledAdornedInputField borderRadius={borderRadius} {...props}>
      {label &&
        (typeof label === 'string' ? (
          <Label p={p} {...labelProps}>
            {label}
          </Label>
        ) : (
          { label }
        ))}
      <Input
        plain
        p={p}
        type={type}
        value={value}
        onChange={onChange}
        {...inputProps}
      />
      {icon &&
        (typeof icon === 'string' ? (
          <IconButton p={p} name={icon} size={32} {...iconProps} />
        ) : (
          { icon }
        ))}
    </StyledAdornedInputField>
  );
};

AdornedInputField.displayName = 'AdornedInputField';
AdornedInputField.propTypes = {
  icon: PropTypes.oneOfType([PropTypes.string, PropTypes.element]),
  iconPlacement: PropTypes.oneOf(['left', 'right']),
  label: PropTypes.oneOfType([PropTypes.string, PropTypes.element]),
  labelPlacement: PropTypes.oneOf(['left', 'right']),
  ...StyledAdornedInputField.propTypes,
};

export default AdornedInputField;
