import React from 'react';
import { shallow } from 'enzyme';
import toJson from 'enzyme-to-json';

import MultiSelect from '../MultiSelect';

const options = [
  { value: 'fishing', label: 'Fishing' },
  { value: 'scuba', label: 'Scuba' },
  { value: 'climbing', label: 'Climbing' },
  { value: 'hunting', label: 'Hunting' },
];

describe('MultiSelect component', () => {
  it('renders initial state correctly', () => {
    const localProps = {
      options,
      selected: [],
      label: 'Activities...',
      onSelect: () => {},
      onUnselect: () => {},
      onAdd: () => {},
      onSearch: () => {},
    };
    const wrapper = shallow(<MultiSelect {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('renders added items correctly', () => {
    const localProps = {
      options,
      selected: [options[1], options[2]],
      label: 'Activities...',
      onSelect: () => {},
      onUnselect: () => {},
      onAdd: () => {},
      onSearch: () => {},
    };
    const wrapper = shallow(<MultiSelect {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
