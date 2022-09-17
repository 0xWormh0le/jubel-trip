import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import { mountWithTheme } from '../../utils/testHelpers';
import Input from '../Input';
import Form from '../Form';

describe('Input component', () => {
  it('should render as Input component', () => {
    const onChange = jest.fn();
    const wrapper = mountWithTheme(
      <Form>
        <Input onChange={onChange} />
      </Form>
    );
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('should render with default value', () => {
    const onChange = jest.fn();
    const wrapper = mountWithTheme(
      <Input value="Testing component" onChange={onChange} />
    );
    expect(wrapper.find('input').prop('value')).toEqual('Testing component');
  });

  it('should simulate change of input value', () => {
    const onChange = jest.fn();
    const component = mount(<Input onChange={onChange} />);
    component.simulate('change', { target: { value: 'Testing value' } });
    expect(onChange).toHaveBeenCalledWith('Testing value');
  });
});
