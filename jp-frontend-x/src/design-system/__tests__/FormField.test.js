import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import FormField from '../FormField';
import Input from '../Input';

describe('FormField component', () => {
  const getComponent = (props) => (
    <FormField>
      <Input {...props} py={3} is="input" type="input" />
    </FormField>
  );
  it('should render as FormField correctly', () => {
    const component = getComponent({
      id: 'idFiled',
      name: 'fieldName',
    });
    const wrapper = mount(component);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
