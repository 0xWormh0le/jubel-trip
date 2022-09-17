import styled from '@emotion/styled';
import React from 'react';
import { themeGet } from 'styled-system';

import system from '../utils/System';
import Flex from './Flex';
import Icon from './Icon';

const ClickableIcon = styled(Icon)`
  pointer-events: none;
`;

const SelectBase = system(
  'SelectBase',
  {
    as: 'select',
    fontSize: 1,
    m: 0,
    borderRadius: 'sm',
    border: 1,
    p: 2,
    bg: 'transparent',
  },
  ({ theme }) => ({
    appearance: 'none',
    cursor: 'pointer',
    marginRight: `calc(-${theme.space[2]} - 24px)`,
    paddingRight: `calc(${theme.space[2]} + ${theme.space[1]} + 24px)`,
    '&:hover': {
      borderColor: themeGet('colors.blue'),
      boxShadow: `0 0 0 1px ${themeGet('colors.blue')}`,
    },
  })
);

export const Select = styled((props) => (
  <Flex alignItems="center" justifyItems="center">
    <SelectBase {...props} />
    <ClickableIcon ml={1} name="arrow-drop-down" color="grays.5" />
  </Flex>
))``;

export default Select;
