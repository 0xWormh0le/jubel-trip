import React from 'react';

import Icon from './Icon';
import IconButton from './IconButton';
import Input from './Input';
import Relative from './Relative';

export const SearchInput = ({
  borderRadius = '17px',
  iconSize = 24,
  width = 1,
  onClear,
  ...props
}) => (
  <Relative width={width}>
    <Input borderRadius={borderRadius} py={2} px={1.5 * iconSize} {...props} />
    <Icon
      name="search"
      size={iconSize}
      position="absolute"
      noPointerEvents
      left={iconSize / 2.8}
      top="50%"
      transform="translate(0, -50%)"
    />
    <IconButton
      name="clear"
      position="absolute"
      right={iconSize / 2.8}
      top="50%"
      transform="translate(0, -50%)"
      onClick={onClear && (() => onClear())}
    />
  </Relative>
);

export default SearchInput;
