import compose from 'lodash/fp/compose';
import { inject, observer } from 'mobx-react';
import React from 'react';
import { withTranslation } from 'react-i18next';

import { Button, Flex, Input } from '../design-system';

const TripSearch = ({ searchText, triggerOnEnter, update, t }) => (
  <Flex width="65%" fontSize={6} alignItems="center">
    <Input
      search
      placeholder={t('main_search_placeholder', 'Where do you want to go?')}
      value={searchText}
      onKeyDown={(event) => triggerOnEnter(event.key)}
      onChange={(searchText) => update(searchText)}
    />
  </Flex>
);

export default compose(
  withTranslation('common'),
  inject(({ app }) => ({
    triggerOnEnter: (key) => app.triggerOnEnter(key),
    searchText: app.searchText,
    update: (searchText) => app.setSearchText(searchText),
  })),
  observer
)(TripSearch);
