import React from 'react';
import { mount } from 'enzyme';
import {
  Heading1,
  Heading2,
  Heading3,
  Heading4,
  Heading5,
  Heading6,
} from '../Heading';

describe('Heading components', () => {
  it('should render h1', () => {
    const component = mount(<Heading1>Heading 1</Heading1>);
    expect(component.find('h1').length).toBe(1);
  });
  it('should render h2', () => {
    const component = mount(<Heading2>Heading 2</Heading2>);
    expect(component.find('h2').length).toBe(1);
  });
  it('should render h3', () => {
    const component = mount(<Heading3>Heading 3</Heading3>);
    expect(component.find('h3').length).toBe(1);
  });
  it('should render h4', () => {
    const component = mount(<Heading4>Heading 4</Heading4>);
    expect(component.find('h4').length).toBe(1);
  });
  it('should render h5', () => {
    const component = mount(<Heading5>Heading 5</Heading5>);
    expect(component.find('h5').length).toBe(1);
  });
  it('should render h6', () => {
    const component = mount(<Heading6>Heading 6</Heading6>);
    expect(component.find('h6').length).toBe(1);
  });
});
