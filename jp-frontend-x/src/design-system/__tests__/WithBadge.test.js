import React from 'react';
import { shallow } from 'enzyme';
import toJson from 'enzyme-to-json';

import WithBadge from '../WithBadge';

describe('WithBadge component', () => {
  it('renders content with badge correctly', () => {
    const localProps = {
      bagdeLabel: '3',
      position: 'right',
      children: <div>Content</div>,
    };
    const wrapper = shallow(<WithBadge {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
