import { PropTypes } from 'prop-types';
import React from 'react';

import system from '../utils/System';
import Flex from './Flex';
import Icon from './Icon';
import Label from './Label';

const lbl = (label, name) =>
  typeof label === 'string' ? label : <Label htmlFor={name}>{label}</Label>;

const iconTypes = {
  checkbox: { checked: 'check-square', unchecked: 'square' },
  radio: { checked: 'dot-circle', unchecked: 'circle' },
  toggle: { checked: 'toggle-on', unchecked: 'toggle-off' },
};

const StyledToggleButton = system(
  'StyledToggleButton',
  {
    extend: Flex,
    alignItems: 'center',
  },
  () => ({
    cursor: 'pointer',
  })
);

const ToggleButton = ({
  id,
  name,
  value = 1,
  label,
  type = 'toggle',
  onChange,
  checked = false,
  size = 32,
  labelPosition = 'after',
  ...props
}) => {
  const icons = iconTypes[type];
  const before = labelPosition === 'before';
  const after = labelPosition === 'after';
  const labelComp = lbl(label, name || id);
  return (
    <StyledToggleButton
      onClick={onChange && (() => onChange(checked ? '' : value))}
      {...props}
    >
      <input
        type="hidden"
        name={name}
        id={id || name}
        value={checked ? value : ''}
        readOnly
      />
      {label && before && labelComp}
      <Icon
        ml={before && 2}
        mr={after && 2}
        name={icons[checked ? 'checked' : 'unchecked']}
        size={size}
      />
      {label && after && labelComp}
    </StyledToggleButton>
  );
};

ToggleButton.displayName = 'ToggleButton';
ToggleButton.propTypes = {
  name: PropTypes.string.isRequired,
  id: PropTypes.string,
  label: PropTypes.oneOfType([PropTypes.string, PropTypes.node]),
  onToggle: PropTypes.func,
  checked: PropTypes.bool,
  value: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
  size: PropTypes.number,
  labelPosition: PropTypes.oneOf(['before', 'after']),
};

export const CheckboxButton = (props) => (
  <ToggleButton {...props} type="checkbox" />
);

export const RadioButton = (props) => <ToggleButton {...props} type="radio" />;

export default ToggleButton;
