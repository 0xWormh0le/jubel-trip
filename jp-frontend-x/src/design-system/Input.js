import React from 'react';

import system from '../utils/System';
import Box from './Box';

// import { themeGet } from 'styled-system';

const StyledInput = system(
  'Input',
  {
    extend: Box,
    as: 'input',
    type: 'text',
    boxShadow: 0,
    borderRadius: 'sm',
    bg: 'surface',
    flex: 1,
    border: 1,
    p: 2,
    width: 1,
    value: '',
  },
  (props) => ({
    appearance: 'none',
    // ':focus': {
    //   border: props.border && themeGet(`borders.${props.border}`)(props),
    // },
    // '::placeholder': {
    //   color: themeGet(`colors.misc.muted`)(props),
    // },
    // '::ms-clear': {
    //   display: 'none',
    // },
  })
);

export const Input = React.forwardRef(
  ({ autoFocus = false, onChange, ...props }, ref) => (
    <StyledInput
      ref={ref}
      autoFocus={autoFocus}
      onChange={onChange ? (event) => onChange(event.target.value) : undefined}
      {...props}
    />
  )
);

export default Input;
