import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import IconButton from '../IconButton';

describe('IconButton component', () => {
  it('should render as IconButton', () => {
    const wrapper = mount(<IconButton />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
  it('should render IconButton with passed props', () => {
    const props = {
      name: 'menu',
    };
    expect(
      mount(<IconButton {...props} />)
        .find(IconButton)
        .prop('name')
    ).toBe('menu');
  });
  it('simulates click events', () => {
    const onClick = jest.fn();

    const button = mount(<IconButton onClick={onClick} />);
    expect(onClick).not.toBeCalled();
    button
      .find(IconButton)
      .first()
      .simulate('click');
    expect(onClick).toBeCalled();
  });
  // it('should render disabled IconButton', () => {
  //   const onClick = jest.fn();
  //   const button = mount(<IconButton disabled onClick={onClick} />);
  //   expect(onClick).not.toBeCalled();
  //   button
  //     .find(IconButton)
  //     .first()
  //     .simulate('click');
  //   expect(onClick).not.toBeCalled();
  // });
});
