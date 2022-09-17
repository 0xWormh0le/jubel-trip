import React from 'react';
import { withTranslation } from 'react-i18next';

import { Flex, TabButton } from '../design-system';

const Tabs = ({
  tabs,
  t,
  fontSize = 3,
  onChange,
  shouldTranslate = true,
  ...props
}) => {
  const handleTabChange = (id) => () => {
    tabs.setActiveTab(id);
    onChange && onChange(id);
  };
  return (
    <Flex fontSize={fontSize} {...props}>
      {tabs.tabs.map(({ id, name, active }) => (
        <TabButton key={id} active={active} onClick={handleTabChange(id)}>
          {shouldTranslate ? t(id, name) : name}
        </TabButton>
      ))}
    </Flex>
  );
};

export default withTranslation('common')(Tabs);
