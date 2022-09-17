import React from 'react';
import { Async as AsyncSelect } from 'react-select';

import Box from './Box';

// const iconAndText = ({
//   label,
//   icon = 'dot',
//   iconSize = 24,
//   iconColor = 'inherit',
// }) => (
//   <Flex alignItems="center" color="on.background" bg="transparent">
//     <Icon name={icon} color={iconColor} size={iconSize} mr={2} />
//     <Text ml>{label}</Text>
//   </Flex>
// );

const RemoteSelect = ({
  loadOptions,
  onChange,
  value,
  clearable = true,
  defaultOptions = true,
  cacheOptions = true,
  multiple = false,
  menuPlacement = 'auto',
  selectProps = {},
  disabled = false,
  placeholder = 'Select...',
  ...props
}) => (
  <Box className="remove-input-focus" {...props}>
    <AsyncSelect
      placeholder={placeholder}
      isMulti={multiple}
      isDisabled={disabled}
      isClearable={clearable}
      cacheOptions={cacheOptions}
      defaultOptions={defaultOptions}
      loadOptions={loadOptions}
      menuPlacement={menuPlacement}
      menuPortalTarget={document.body}
      value={value}
      menuShouldBlockScroll
      styles={{
        menuPortal: (base) => ({ ...base, zIndex: 9999 }),
      }}
      onChange={onChange && ((selected) => onChange(selected))}
      {...selectProps}
    />
  </Box>
);

export default RemoteSelect;
