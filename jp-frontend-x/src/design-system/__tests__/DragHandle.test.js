import React from 'react';
import { mount, shallow } from 'enzyme';
import toJson from 'enzyme-to-json';
import DragHandle from '../DragHandle';

describe('DragHandle component', () => {
  it('should render as DragHandle', () => {
    const wrapper = mount(<DragHandle />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
  it('simulates click events', () => {
    const onClick = jest.fn();
    const localProps = {
      onClick: onClick,
    };
    const wrapper = mount(<DragHandle {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
    wrapper
      .find(DragHandle)
      .first()
      .simulate('click');
    expect(onClick).toBeCalled();
  });

  // it('should render disabled DragHandle', () => {
  //   const onClick = jest.fn();
  //   const wrapper = mount(<DragHandle onClick={onClick} disabled />);
  //   expect(toJson(wrapper)).toMatchSnapshot();
  //   wrapper
  //     .find(DragHandle)
  //     .first()
  //     .simulate('click');

  //   expect(onClick).not.toBeCalled();
  // });
});
