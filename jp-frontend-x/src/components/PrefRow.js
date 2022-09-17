import compose from 'lodash/fp/compose';
import { inject, observer } from 'mobx-react';
import React from 'react';
import { withTranslation } from 'react-i18next';

import { Flex, Relative, Text } from '../design-system';

export const PrefRow = ({
  tkey,
  label,
  value,
  info,
  tipType = 'markdown',
  labelProps = {},
  children,
  t,
  showTips,
  ...props
}) => {
  const dataRh = `data-rh${tipType ? `-${tipType}` : ''}`;
  const tipProps = showTips
    ? {
        [dataRh]: t(`${tkey}_tip`, info),
        [`${dataRh}-at`]: 'left',
      }
    : {};
  return (
    <Relative bg="surface" hoverable borderBottom={1} p={3} {...tipProps}>
      <Text pb={2} bold uppercase fontSize={3} {...labelProps}>
        {t(tkey, label)}
        {value && (
          <Text ml={1} color="misc.muted" as="span">
            : {value}
          </Text>
        )}
      </Text>
      <Flex flexDirection="column" {...props}>
        {children}
      </Flex>
    </Relative>
  );
};

export default compose(
  withTranslation('common'),
  inject(({ app }) => ({
    showTips: app.preferences.showTips,
  })),
  observer
)(PrefRow);
