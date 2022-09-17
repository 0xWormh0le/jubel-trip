import styled from '@emotion/styled';
import PropTypes from 'prop-types';
import React from 'react';
import { components } from 'react-select';
import CreatableSelect from 'react-select/lib/Creatable';

import { selectOptionType } from '../utils/propTypes';
import Box from './Box';
import IconButton from './IconButton';
import Label from './Label';
import theme from './theme';

/**
 *
 * Example Usage
 *
 * <MultiSelect
 *   options={app.optionsFor('activities')}
 *   selected={app.selectedOptionsFor('activities')}
 *   label="Activities..."
 *   direction="ltr"
 *   onSelect={(selected) => app.selectOptionsFor('activities', selected)}
 *   onUnselect={(value) => app.unselectOptionsFor('activities', value)}
 *   onAdd={(input)=> app.addOptionFor('activities', input)}
 *   onSearch={(input) => app.searchOptionFor('activities', input)}
 * />
 *
 **/

/**************************************
 * custom renderer of selected values
 * difficult to customize react-select
 * to match design
 ***************************************/
const SelectedTagWrapper = styled('div')`
  display: flex;
  align-items: center;
  border: 1px solid ${theme.colors.selects.tagBorder};
  margin: 0.25rem;
  padding: 0 0.25rem;
`;

const SelectedTagsContainer = styled('div')`
  display: flex;
  flex-wrap: wrap;
  margin-top: 0.5rem;
  margin-left: -0.25rem;
`;

function SelectedTag({ data: { value, label }, onRemove }) {
  const handleRemove = () => {
    onRemove(value);
  };

  return (
    <SelectedTagWrapper>
      <IconButton
        size={14}
        name="clear"
        p={1}
        onClick={handleRemove}
        color={theme.colors.on.surface[0]}
      />
      <Label fontSize={4} fontWeight="normal" color="on.surface" p={1}>
        {label}
      </Label>
    </SelectedTagWrapper>
  );
}

/*************************************
 * customized styles for react-select
 **************************************/
const stylesForSelect = {
  placeholder: (styles, { isFocused }) => ({
    ...styles,
    color: isFocused ? theme.colors.on.surface[2] : theme.colors.on.surface[1],
  }),
  option: (styles, { isFocused }) => ({
    ...styles,
    cursor: 'pointer',
    padding: '0.5rem 1rem',
    backgroundColor: isFocused
      ? theme.colors.selects.optionFocusBg
      : theme.colors.surface[1],
    color: isFocused
      ? theme.colors.selects.primaryColor
      : theme.colors.on.surface[1],
  }),
  menu: (styles) => ({
    ...styles,
    marginTop: '2px',
    border: `1px solid ${theme.colors.primaryGray}`,
    borderRadius: 0,
    boxShadow: `0 1px 3px 0 ${theme.colors.selects.menuShadow}`,
  }),
  valueContainer: (styles) => ({
    ...styles,
    paddingLeft: 0,
    paddingRight: 0,
  }),
  control: (styles, { isFocused, menuIsOpen }) => ({
    ...styles,
    border: 'none',
    cursor: isFocused ? 'text' : 'pointer',
    borderRadius: 0,
    boxShadow: menuIsOpen
      ? `0 2px 0 0 ${theme.colors.selects.primaryColor}`
      : isFocused
      ? `0 1px 0 0 ${theme.colors.selects.primaryColor}`
      : `0 1px 0 0 ${theme.colors.primaryGray}`,
  }),
  dropdownIndicator: (styles, data) => ({
    ...styles,
    padding: 0,
    color: data.selectProps.menuIsOpen
      ? theme.colors.on.surface[2]
      : theme.colors.primaryGray,
  }),
  noOptionsMessage: (styles) => ({
    ...styles,
    color: theme.colors.on.surface[1],
  }),
};

const DropdownIndicator = (props) => (
  <components.DropdownIndicator {...props}>
    <IconButton
      size={24}
      name={props.selectProps.menuIsOpen ? 'arrow-drop-up' : 'arrow-drop-down'}
      px={0}
    />
  </components.DropdownIndicator>
);

/***************************************
 * main component rendering react-select
 ****************************************/
function ReactSelectWrapper({
  selected,
  label,
  direction,
  onSelect,
  onUnselect,
  onAdd,
  onSearch,
  ...rest
}) {
  const handleInputChange = (value, { action }) => {
    if (action === 'input-change') {
      onSearch(value);
    }
  };

  return (
    <Box>
      <CreatableSelect
        {...rest}
        placeholder={label}
        value={selected}
        isRtl={direction !== 'ltr'}
        onChange={onSelect}
        onCreateOption={onAdd}
        onInputChange={handleInputChange}
        isMulti
        styles={stylesForSelect}
        controlShouldRenderValue={false}
        backspaceRemovesValue={false}
        components={{
          ClearIndicator: null,
          IndicatorSeparator: null,
          DropdownIndicator,
        }}
      />

      <SelectedTagsContainer>
        {selected.map((tag) => (
          <SelectedTag key={tag.value} data={tag} onRemove={onUnselect} />
        ))}
      </SelectedTagsContainer>
    </Box>
  );
}

// const MultiSelect = styled(ReactSelectWrapper)``;
const MultiSelect = ReactSelectWrapper;

MultiSelect.propTypes = {
  options: PropTypes.arrayOf(selectOptionType).isRequired,
  selected: PropTypes.arrayOf(selectOptionType).isRequired,
  label: PropTypes.string,
  direction: PropTypes.string,
  onSelect: PropTypes.func.isRequired,
  onAdd: PropTypes.func,
  onSearch: PropTypes.func,
};

MultiSelect.defaultProps = {
  label: 'Select...',
  direction: 'ltr',
};

export default MultiSelect;
