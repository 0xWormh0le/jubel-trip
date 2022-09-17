import React, { useCallback, useEffect, useRef, useState } from 'react';
import { withTranslation, Trans } from 'react-i18next';
import compose from 'lodash/fp/compose';
import throttle from 'lodash/throttle';

import {
  Box,
  Button,
  Flex,
  FlexPanel,
  Image,
  Text,
  RemoteSelect,
} from '../design-system';
import { inject, observer } from 'mobx-react';
import { jubelLogo } from '../assets';
import Screen from '../components/Screen';

const NEXT_TRIP_INTERVAL = 6700;

const Search = ({
  thisCoolPlace,
  thisCoolPlaceValue,
  searchValue,
  update,
  t,
  randomTrips,
  resetTripsMode,
  nextPlace,
  boundingRect,
  history,
  loadOptions,
  searchBoxTop,
  setSearchBoxHeight,
}) => {
  const [placeIndex, setPlaceIndex] = useState(0);

  const el = useRef(null);

  const handleNotSure = useCallback(() => {
    history.push('/trips');
  }, [history]);

  useEffect(() => {
    if (randomTrips.length > 0) {
      nextPlace(randomTrips[placeIndex], boundingRect);
    }
  }, [placeIndex, randomTrips]); // eslint-disable-line

  useEffect(() => {
    const timerId = setInterval(() => {
      setPlaceIndex(
        (placeIndex) => (placeIndex + 1) % (randomTrips.length || 1)
      );
    }, NEXT_TRIP_INTERVAL);
    return () => clearInterval(timerId);
  }, [randomTrips]);

  useEffect(() => {
    resetTripsMode();
    return () => {};
  }, [resetTripsMode]);

  useEffect(() => {
    const updateSearcboxHeight = () => {
      if (el) {
        setSearchBoxHeight(el.current.offsetHeight);
      }
    };

    const eventHandler = throttle(updateSearcboxHeight, 200);
    window.addEventListener('resize', eventHandler);
    updateSearcboxHeight();
    return () => {
      window.removeEventListener('resize', eventHandler);
    };
  }, [el, setSearchBoxHeight]);

  return (
    <Screen
      name="search"
      full
      justifyContent="space-between"
      alignItems="center"
      bg="misc.map.overlay"
      paddingTop={searchBoxTop}
    >
      <FlexPanel
        ref={el}
        bg="surface"
        py={4}
        px={5}
        borderRadius="md"
        boxShadow={6}
        opacity={0.85}
        color="on.surface"
        alignItems="center"
        overflow="auto"
      >
        <Image hoverable src={jubelLogo} alt="Jubel" height={80} />
        <Text mt={3} fontSize={8} center>
          <b>{t('main_headline_1', 'Your Perfect Trip - Hassle Free')}</b>
        </Text>
        <Text
          mt={3}
          mb={4}
          fontSize={6}
          center
          maxWidth={500}
          lineHeight="copy"
        >
          {t(
            'main_headline_2',
            'Discover extraordinary trips that match your style. Then, easily tailor & book them with an expert.'
          )}
        </Text>
        <Box width={410}>
          <RemoteSelect
            placeholder={t(
              'main_search_placeholder',
              'Where do you want to go?'
            )}
            value={searchValue || null}
            cacheOptions
            defaultOptions
            loadOptions={loadOptions('placesToSee')}
            onChange={update}
            selectProps={{
              autoFocus: true,
              components: {
                DropdownIndicator: null,
              },
              styles: {
                placeholder: (provided) => ({
                  ...provided,
                  fontWeight: 'bold',
                }),
              },
            }}
          />
        </Box>
        <Text
          onClick={() => update({ value: thisCoolPlaceValue })}
          hoverable
          bg="surface"
          fontSize={4}
          my={1}
          p={3}
          color="misc.muted"
        >
          {/* prettier-ignore */}
          <Trans i18nKey="how_about_this_cool_place">
            How about ... <Text
              inline
              style={{textDecoration: "underline"}}
              fontWeight="bold"
            >
              {{ thisCoolPlace }}?
            </Text>
          </Trans>
        </Text>
        <Flex>
          <Button
            fontSize={4}
            secondary
            px={4}
            py={3}
            fontWeight="bold"
            onClick={handleNotSure}
          >
            {t('not_sure_find', "Not Sure? Find Places You'll Love...")}
          </Button>
        </Flex>
      </FlexPanel>
    </Screen>
  );
};

const SearchWrapper = (props) => <Search {...props} />;

export default compose(
  withTranslation('common'),
  inject(({ app }) => ({
    randomTrips: app.preferences.randomTrips,
    nextPlace: app.map.nextPlace,
    boundingRect: app.boundingRect,
    thisCoolPlace: app.map.placeName,
    thisCoolPlaceValue: app.map.placeValue,
    resetTripsMode: app.resetTripsMode,
    searchValue: app.searchValue,
    update: app.setSearchValue,
    loadOptions: app.preferences.loadOptions,
    setSearchBoxHeight: app.setSearchBoxHeight,
    searchBoxTop: app.headerHeight,
  })),
  observer
)(SearchWrapper);
