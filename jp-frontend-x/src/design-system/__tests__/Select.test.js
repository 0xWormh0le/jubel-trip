import React from 'react';
import { mount, shallow } from 'enzyme';
import toJson from 'enzyme-to-json';
import Select from '../Select';

describe('Select component', () => {
  const getComponent = (props) => (
    <Select>
      <option value="1">1</option>
      <option value="2">2</option>
    </Select>
  );
  it('should render as Select component', () => {
    const wrapper = shallow(<Select />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
  it('should render a select component with 2 options', () => {
    const component = getComponent();
    expect(component.props.children.length).toBe(2);
  });
});
