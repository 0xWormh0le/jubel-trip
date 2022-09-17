/* eslint-disable import/first */
import React from 'react';
import { storiesOf } from '@storybook/react';
import { withTranslation } from 'react-i18next';

import Box from '../design-system/Box';
import Button from '../design-system/Button';
import FlexPanel from '../design-system/FlexPanel';
import Text from '../design-system/Text';

const I18next = ({ t, i18n }) => (
  <Box>
    <Box>
      <Button onClick={() => i18n.changeLanguage('en-US')}>en-US</Button>
      <Button onClick={() => i18n.changeLanguage('de-DE')}>de-DE</Button>
      <Button onClick={() => i18n.changeLanguage('es-MX')}>es-MX</Button>
      <Button onClick={() => i18n.changeLanguage('es-ED')}>es-ED</Button>
      <Button onClick={() => i18n.changeLanguage('jp-JP')}>jp-JP</Button>
      <Button onClick={() => i18n.changeLanguage('fr-FR')}>fr-FR</Button>
    </Box>
    <Box p={3}>
      <Text center bold p={3}>
        {t('story_some_text', 'Some default text')}
      </Text>
      <Text uppercase center bold p={3}>
        Text translation:{' '}
        {t('story_regular_text', 'Default value, second parameter in t')}
      </Text>{' '}
      <Text center bold>
        Date: {t('story_date', { date: new Date() })}
      </Text>
      <Text center bold>
        Number: {t('story_number', { number: 1225342 })}
      </Text>
      <Text center bold>
        Dollars: {t('dollar_amount', { amount: 2352233 })}
      </Text>
      <Text center bold>
        Local Currency: {t('local_amount', { amount: 2352233 })}
      </Text>
    </Box>
  </Box>
);

const TranslatedI18next = withTranslation('common')(I18next);

storiesOf('I18next', module)
  .addDecorator((story) => (
    <FlexPanel alignItems="center">
      <Text p={3}>i18next demo</Text>
      {story()}
    </FlexPanel>
  ))
  .add('default', () => <TranslatedI18next />, {
    notes: {
      markdown: '#### I18next demonstration story',
    },
  });
