import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import TabButton from '../TabButton';
import TabButtonContainer from '../TabButtonContainer';

describe('TabButtonContainer component', () => {
  const getComponent = (props) => (
    <TabButtonContainer>
      <TabButton>Button</TabButton>
    </TabButtonContainer>
  );
  it('should render TabButtonContainer', () => {
    const wrapper = mount(<TabButtonContainer />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
  it('should render TabButtonContainer with children', () => {
    const component = getComponent();
    const wrapper = mount(component);
    expect(wrapper.find(TabButton).length).toBe(1);
  });
});
