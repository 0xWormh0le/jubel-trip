import React from 'react';
import { mount, shallow } from 'enzyme';
import toJson from 'enzyme-to-json';

import { DropDownButton, MenuList, ListItem } from '../DropDownButton';
import IconButton from '../IconButton';
import Relative from '../Relative';

const items = [
  {
    value: 'byName',
    label: 'by Name',
  },
  {
    value: 'byDate',
    label: 'by Date',
  },
  {
    value: 'byAge',
    label: 'by Age',
  },
];

describe('DropDownButton component', () => {
  it('renders correctly', () => {
    const localProps = {
      t: (t) => t,
      label: 'Sort by',
      color: 'on.surface',
      value: null,
      onSelect: () => {},
      items,
    };
    const wrapper = mount(<DropDownButton {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
    expect(wrapper.find(IconButton).prop('label')).toEqual(localProps.label);
  });

  it('opens or hides the dropdown menu with user interaction', () => {
    const localProps = {
      t: (t) => t,
      label: 'Sort by',
      color: 'on.surface',
      value: 'byName',
      onSelect: jest.fn(),
      items,
    };
    const wrapper = mount(<DropDownButton {...localProps} />);

    expect(wrapper.find(MenuList)).toHaveLength(0);

    wrapper.find(IconButton).simulate('click');
    expect(wrapper.find(MenuList)).toHaveLength(1);

    wrapper.find(Relative).simulate('keydown', { keyCode: 13 });
    expect(wrapper.find(MenuList)).toHaveLength(1);

    wrapper.find(Relative).simulate('keydown', { keyCode: 13 });
    wrapper.find(Relative).simulate('keydown', { keyCode: 27 });
    expect(wrapper.find(MenuList)).toHaveLength(0);
  });

  it('triggers event handler prop functions when dropdown item is selected', () => {
    const localProps = {
      t: (t) => t,
      label: 'Sort by',
      color: 'on.surface',
      value: 'byName',
      onSelect: jest.fn(),
      items,
    };
    const wrapper = mount(<DropDownButton {...localProps} />);
    wrapper.find(IconButton).simulate('click');
    wrapper
      .find(MenuList)
      .find(ListItem)
      .at(0)
      .simulate('click');
    expect(localProps.onSelect).toHaveBeenCalledWith(items[0]);
  });

  it('on keydown select item', () => {
    const localProps = {
      t: (t) => t,
      label: 'Sort by',
      color: 'on.surface',
      value: 'byDate',
      onSelect: jest.fn(),
      items,
    };
    const wrapper = mount(<DropDownButton {...localProps} />);
    wrapper
      .find(Relative)
      .simulate('keydown', { keyCode: 13 })
      .simulate('keydown', { keyCode: 40 })
      .simulate('keydown', { keyCode: 40 })
      .simulate('keydown', { keyCode: 13 });
    expect(localProps.onSelect).toHaveBeenCalledWith(items[1]);
    expect(wrapper.find(MenuList)).toHaveLength(0);
  });
});
