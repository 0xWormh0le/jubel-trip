import { inject, observer } from 'mobx-react';
import { withRouter } from 'react-router';
import { withTranslation } from 'react-i18next';
import compose from 'lodash/fp/compose';
import React from 'react';

import { flagEmoji } from '../design-system/theme/utils';
import { jubelLogo } from '../assets';
import shadows from '../design-system/theme/shadows';

import {
  Absolute,
  Flex,
  Image,
  Text,
  SimpleSelect,
  Link,
} from '../design-system';
import Breadcrumbs from './Breadcrumbs';
import NetworkStatus from './NetworkStatus';

const countryCode = (code) => {
  const c_code = code.split('-');
  return c_code[1];
};

const countryFlag = (locale, t) => {
  return (
    <Flex alignItems="center">
      <Text fontSize={6} minWidth={20} center>
        {flagEmoji(countryCode(locale.key))}
      </Text>
      <Text pl={3}>{t(locale.key, locale.value)}</Text>
    </Flex>
  );
};

export const MainHeader = ({
  locales,
  t,
  i18n,
  headerHeight,
  isSearchScreen,
}) => (
  <Absolute
    className="main-header"
    position="fixed"
    left={0}
    top={0}
    width={1}
    zIndex={999}
    height={headerHeight}
    boxShadow={isSearchScreen ? undefined : 1}
    bg={isSearchScreen ? undefined : 'surface'}
  >
    <Flex
      justifyContent="space-between"
      height={headerHeight}
      width={1}
      flexDirection="row"
      alignItems="center"
    >
      {!isSearchScreen && (
        <Flex alignItems="center" ml={2} mr={3}>
          <Link to="/" display="block">
            <Image src={jubelLogo} height={48} width="auto" display="block" />
          </Link>
        </Flex>
      )}
      <Flex flex={1} ml={2}>
        {!isSearchScreen && <Breadcrumbs />}
      </Flex>
      <Flex ml={3} mr={2}>
        <SimpleSelect
          optionsStyle={{
            fontSize: 4,
          }}
          px={2}
          clearable={false}
          width={150}
          selectProps={{
            styles: {
              menuPortal: (provided) => ({
                ...provided,
                zIndex: 9999,
              }),
              control: (provided) => ({
                ...provided,
                ...(isSearchScreen ? { boxShadow: shadows[6] } : {}),
              }),
            },
          }}
          options={locales.map((option) => ({
            key: option.key,
            label: countryFlag(option, t),
          }))}
          value={locales
            .filter((loc) => loc.key === i18n.language)
            .map((loc) => ({
              value: loc.key,
              label: countryFlag(loc, t),
            }))}
          onChange={(event) => i18n.changeLanguage(event.key)}
        />
      </Flex>
    </Flex>
    <Flex alignItems="center" mt={1}>
      <NetworkStatus />
    </Flex>
  </Absolute>
);

export default compose(
  withRouter,
  withTranslation('common'),
  inject(({ app }) => ({
    locales: app.locales,
    toggleTrips: () => app.toggleTrips(),
    headerHeight: app.headerHeight,
    isSearchScreen: app.isSearchScreen,
  })),
  observer
)(MainHeader);
