import compose from 'lodash/fp/compose';
import { inject, observer } from 'mobx-react';
import React from 'react';
import { withTranslation } from 'react-i18next';

import { FlexPanel, Text } from '../design-system';

const ScorableAlternativeAlternatives = ({ element, t }) => (
  <FlexPanel bg="background" fontSize={4} flex={1}>
    <Text>Alternatives</Text>
  </FlexPanel>
);

export default compose(
  withTranslation('common'),
  inject(({ app }) => ({
    element: app.selectedRouteElement,
  })),
  observer
)(ScorableAlternativeAlternatives);
