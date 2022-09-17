import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';

import Button from '../Button';

describe('Button component', () => {
  it('renders default button correctly', () => {
    const wrapper = mount(<Button>Click Me</Button>);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('renders active button correctly', () => {
    const wrapper = mount(<Button active>Active</Button>);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('renders primary button correctly', () => {
    const wrapper = mount(<Button primary>Primary</Button>);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('renders secondary button correctly', () => {
    const wrapper = mount(<Button secondary>Secondary</Button>);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('renders info button correctly', () => {
    const wrapper = mount(<Button info>Info</Button>);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('renders info button correctly', () => {
    const wrapper = mount(<Button info>Info</Button>);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('renders error button correctly', () => {
    const wrapper = mount(<Button error>Error</Button>);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('renders warning button correctly', () => {
    const wrapper = mount(<Button warning>Warning</Button>);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('renders warning button correctly', () => {
    const wrapper = mount(<Button disabled>Disabled</Button>);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
