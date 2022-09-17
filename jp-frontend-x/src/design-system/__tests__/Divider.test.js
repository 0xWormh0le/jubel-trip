import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';

import Divider from '../Divider';
import Relative from '../Relative';

describe('Divider component', () => {
  it('renders horizontal divider correctly', () => {
    const wrapper = mount(<Divider text="OR" width={1} bg="surface" />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('renders vertical divider correctly', () => {
    const wrapper = mount(
      <Divider borderColor="primary" vertical thickness={2} height={150}>
        <Relative p={2} borderRadius="sm" primary uppercase>
          then
        </Relative>
      </Divider>
    );
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
