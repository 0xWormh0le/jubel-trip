import React from 'react';

import Grid from './Grid';
import system from '../utils/System';

const StyledForm = system('StyledForm', {
  extend: Grid,
  as: 'form',
  gridGap: '0.5rem',
  gridTemplateColumns: 'auto 1fr',
});

const Form = ({ children, ...props }) => (
  <StyledForm {...props}>{children}</StyledForm>
);

export default Form;
