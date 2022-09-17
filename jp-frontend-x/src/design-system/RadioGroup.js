import React from 'react';

import IconButton from './IconButton';

const RadioGroup = ({ value, options, onChange, size = 24, ...otherProps }) =>
  options.map((label, option) => {
    const checked = option === value;
    return (
      <IconButton
        key={option}
        name={checked ? 'radio-button-checked' : 'radio-button-unchecked'}
        size={size}
        label={label}
        onClick={onChange && (() => onChange(option))}
        {...otherProps}
      />
    );
  });
RadioGroup.displayName = 'RadioGroup';

export default RadioGroup;
