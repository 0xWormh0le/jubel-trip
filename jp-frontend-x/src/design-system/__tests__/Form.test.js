import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import Input from '../Input';
import Form from '../Form';

describe('Form component', () => {
  it('should render form component', () => {
    const wrapper = mount(<Form />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
  it('should render component with children', () => {
    const onChange = jest.fn();
    const component = mount(
      <Form>
        <Input onChange={onChange} />
      </Form>
    );
    expect(component.find(Input).exists()).toBe(true);
  });
  it('should simulate submit', () => {
    const fakeSubmit = jest.fn();
    const onChange = jest.fn();
    const component = mount(
      <Form onSubmit={fakeSubmit}>
        <Input onChange={onChange} />
      </Form>
    );
    component.find('form').simulate('submit');
    expect(fakeSubmit).toBeCalled();
  });
});
