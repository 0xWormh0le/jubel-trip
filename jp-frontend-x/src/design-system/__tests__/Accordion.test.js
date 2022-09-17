import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import mockDate from '../../__mocks__/dateMocks';

import Accordion from '../Accordion';

describe('Accordion component', () => {
  let timestamp;
  const getComponent = (props) => (
    <Accordion {...props}>
      <Accordion.Section name="section1">
        <Accordion.Heading visibleWhen="collapsed">
          Collapsed Heading 1
        </Accordion.Heading>
        <Accordion.Heading visibleWhen="expanded">
          Expanded Heading 1
        </Accordion.Heading>
        <Accordion.Content>Accordion Content 2</Accordion.Content>
      </Accordion.Section>
      <Accordion.Section name="section2">
        <Accordion.Heading visibleWhen="collapsed">
          Collapsed Heading 2
        </Accordion.Heading>
        <Accordion.Heading visibleWhen="expanded">
          Expanded Heading 2
        </Accordion.Heading>
        <Accordion.Content>Accordion Content 2</Accordion.Content>
      </Accordion.Section>
    </Accordion>
  );

  beforeEach(() => {
    timestamp = mockDate();
  });

  it('renders accordion correctly', () => {
    const component = getComponent({
      name: 'Accordion',
      onSectionToggle: () => {},
      openedSection: 'section2',
    });
    const wrapper = mount(component);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('renders the content of active section only', () => {
    const component = getComponent({
      name: 'Accordion',
      onSectionToggle: () => {},
      openedSection: 'section2',
    });
    const wrapper = mount(component);
    expect(
      wrapper
        .find(Accordion.Section)
        .at(0)
        .find(Accordion.Content)
        .prop('open')
    ).toBe(false);
    expect(
      wrapper
        .find(Accordion.Section)
        .at(1)
        .find(Accordion.Content)
        .prop('open')
    ).toBe(true);
  });

  it('triggers `onSectionToggle` when section header is clicked', () => {
    const accordionProps = {
      name: 'Accordion',
      onSectionToggle: jest.fn(),
      openedSection: 'section2',
    };
    const component = getComponent(accordionProps);
    const wrapper = mount(component);
    wrapper
      .find(Accordion.Section)
      .first()
      .find(Accordion.Heading)
      .first()
      .simulate('click');
    expect(accordionProps.onSectionToggle).toHaveBeenCalled();
  });
});
