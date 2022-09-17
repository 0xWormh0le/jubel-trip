import React, { useCallback, useState } from 'react';
import { inject, observer } from 'mobx-react';
import { Trans, withTranslation } from 'react-i18next';
import compose from 'lodash/fp/compose';

import {
  Box,
  Button,
  CheckBox,
  Flex,
  Grid,
  IconButton,
  Input,
  Label,
  Link,
  Text,
} from '../design-system';
import { stopEvent } from '../utils/events';
import McEmbeddableWidget from './McEmbeddableWidget';

const GetQuoteForm = ({
  update,
  firstName,
  lastName,
  email,
  optIn,
  send,
  close,
  t,
}) => {
  const [emailMode, setEmailMode] = useState(false);
  const handleSetEmailMode = useCallback(
    (event) => {
      event.preventDefault();
      setEmailMode(true);
    },
    [setEmailMode]
  );

  return (
    <Box
      maxWidth={emailMode ? 600 : 400}
      minWidth={emailMode ? 480 : 400}
      width="60vw"
      borderRadius="sm"
    >
      <Text
        py={3}
        bg="background"
        borderRadius="top.sm"
        center
        bold
        borderBottom={1}
      >
        {t('customize_get_a_quote', 'Customize and Get Quote')}
      </Text>
      {emailMode ? (
        <>
          <Flex
            fontSize={4}
            flexDirection="column"
            alignItems="center"
            px={4}
            py={3}
          >
            <Text lineHeight="copy" mb={4}>
              {t(
                'provideContact',
                'Have an expert tailor this trip for you now. Receive a custom-made travel plan that can easily be booked with one-click.'
              )}
            </Text>
            <Grid
              alignItems="center"
              gridTemplateColumns="auto 1fr"
              gridGap={3}
              width="70%"
            >
              <Label nobr textAlign="right">
                {t('first_name', 'First Name')}
              </Label>
              <Input
                required
                onChange={(firstName) => update({ firstName })}
                value={firstName}
              />
              <Label nobr textAlign="right">
                {t('last_name', 'Last Name')}
              </Label>
              <Input
                required
                onChange={(lastName) => update({ lastName })}
                value={lastName}
              />
              <Label nobr textAlign="right">
                {t('email', 'Email')}
              </Label>
              <Input
                required
                onChange={(email) => update({ email })}
                value={email}
                type="email"
              />
            </Grid>
            <Text lineHeight="copy" mt={4} width={1}>
              <Trans i18nKey="confirmGetAQuoteLabel">
                By clicking the <b>{t('get_quote', 'Get Quote')}</b> button
                below you consent to receiving communications from Jubel about
                your itinerary.
              </Trans>
            </Text>
            <Text lineHeight="copy" my={2} width={1}>
              {/* prettier-ignore */}
              <Trans i18nKey="privacyPolicyLabel">
                  See our <Link target="_blank" href="/privacy-policy">{t(
                    'privacy_policy', 'privacy policy'
                  )}</Link> to learn how we use your personal data.
                </Trans>
            </Text>
            <CheckBox
              mt={2}
              mb={4}
              color="on.surface"
              labelUppercase={false}
              labelBold={false}
              labelFontSize={4}
              onChange={(optIn) => update({ optIn })}
              checked={optIn}
              labelMargin={2}
              labelNobr={false}
              label={t(
                'opt_in_checkbox',
                'Opt-in for additional curated travel communications from Jubel.'
              )}
            />
          </Flex>
          <Flex
            bg="background"
            borderRadius="bottom.sm"
            borderTop={1}
            justifyContent="space-between"
            p={3}
            fontSize={4}
          >
            <IconButton
              p={2}
              bg="surface"
              onClick={close}
              name="arrow-back"
              border={1}
              borderRadius="sm"
              iconColor="on.background"
            />
            <Button
              px={3}
              onClick={(event) => {
                if (send()) {
                  close();
                }
                stopEvent(event);
              }}
              primary
            >
              <Text bold uppercase>
                {t('customize_get_a_quote', 'Customize and Get Quote')}
              </Text>
            </Button>
          </Flex>
        </>
      ) : (
        <Flex
          fontSize={4}
          flexDirection="column"
          alignItems="center"
          justifyContent="space-evenly"
          px={4}
          py={5}
        >
          <Box pr={37} mb={3}>
            <McEmbeddableWidget
              widgetId={process.env.REACT_APP_MC_MESSENGER_WIDGET_ID}
            />
          </Box>
          <Link
            tabIndex={0}
            href="#"
            onClick={handleSetEmailMode}
            textDecoration="underline"
          >
            Or Continue With E-Mail
          </Link>
        </Flex>
      )}
    </Box>
  );
};

const GetQuoteFormWrapper = (props) => <GetQuoteForm {...props} />;

export default compose(
  withTranslation('common'),
  inject(({ app }) => ({
    update: (data) => app.customer.update(data),
    firstName: app.customer.firstName,
    lastName: app.customer.lastName,
    email: app.customer.email,
    optIn: app.customer.optIn,
    send: () => app.customer.send(app.currentTrip),
  })),
  observer
)(GetQuoteFormWrapper);
