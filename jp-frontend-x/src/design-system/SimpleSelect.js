import React from 'react';
import ReactSelect from 'react-select';

import Box from './Box';
import { rem2px } from './theme';
import theme from './theme/index';

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

const SimpleSelect = ({
  options,
  onChange,
  value,
  multiple = false,
  clearable = true,
  menuPlacement = 'auto',
  selectProps = {},
  disabled = false,
  placeholder = 'Select...',
  optionsStyle = { fontSize: 4, px: 3, pb: 2 },
  ...props
}) => (
  <Box className="remove-input-focus" {...props}>
    <ReactSelect
      isDisabled={disabled}
      isMulti={multiple}
      isClearable={clearable}
      options={options}
      placeholder={placeholder}
      value={value}
      menuPlacement={menuPlacement}
      menuPortalTarget={document.body}
      menuShouldBlockScroll
      styles={{
        option: () => ({
          fontSize: rem2px(theme.fontSizes[optionsStyle.fontSize]),
          paddingLeft: rem2px(theme.space[3]),
          paddingRight: rem2px(theme.space[3]),
          paddingBottom: rem2px(theme.space[1]),
          paddingTop: rem2px(theme.space[1]),
          cursor: 'pointer',
        }),
        menuPortal: (base) => ({ ...base, zIndex: 9999 }),
      }}
      onChange={onChange && ((selected) => onChange(selected))}
      {...selectProps}
    />
  </Box>
);

export default SimpleSelect;
