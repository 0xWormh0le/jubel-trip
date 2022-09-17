import React from 'react';
import { mount, shallow } from 'enzyme';
import toJson from 'enzyme-to-json';

import DragHandle from '../DragHandle';
import SlidingPanel, { AnimatedMaxPanel, getValidWidth } from '../SlidingPanel';
import ScreenOverlay from '../ScreenOverlay';
import mockDate from '../../__mocks__/dateMocks';

describe('SlidingPanel component', () => {
  let timestamp;
  beforeEach(() => {
    timestamp = mockDate(new Date(2019, 6, 6));
  });

  it('renders horizontal SlidingPanel correctly', () => {
    const localProps = {
      minWidth: 80,
      maxWidth: 800,
      snapWidths: [200, 400, 600],
      resizable: true,
    };
    const wrapper = shallow(<SlidingPanel {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('renders screen overlay while resizing', () => {
    const localProps = {
      minWidth: 80,
      maxWidth: 800,
      snapWidths: [200, 400, 600],
      resizable: true,
    };
    const wrapper = mount(<SlidingPanel {...localProps} />);
    expect(wrapper.find(ScreenOverlay).length).toBe(0);
    wrapper.find(DragHandle).simulate('mousedown');
    expect(wrapper.find(ScreenOverlay).length).toBe(1);
  });

  it('opens sliding panel in hover toggle mode when mouse enters the panel', () => {
    const localProps = {
      open: false,
      onToggle: jest.fn(),
      minWidth: 40,
      maxWidth: 420,
      toggleOn: 'hover',
      closeType: 'clear',
    };

    const wrapper = mount(<SlidingPanel {...localProps} />);
    wrapper.find(AnimatedMaxPanel).simulate('mouseenter');
    expect(localProps.onToggle).toHaveBeenCalledTimes(1);
  });

  describe('getValidWidth helper function', () => {
    it('returns a new panel width while dragging and target width for grid line based on direction', () => {
      const widths = [100, 200, 300];
      let result;
      result = getValidWidth(90, 'left', widths);
      expect(result).toEqual({ newWidth: 100, targetWidth: null });
      result = getValidWidth(90, 'right', widths);
      expect(result).toEqual({ newWidth: 100, targetWidth: 200 });
      result = getValidWidth(150, 'left', widths);
      expect(result).toEqual({ newWidth: 200, targetWidth: 100 });
      result = getValidWidth(150, 'right', widths);
      expect(result).toEqual({ newWidth: 200, targetWidth: null });
    });
  });
});
