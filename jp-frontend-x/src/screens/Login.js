import React from 'react';
import { inject, observer } from 'mobx-react';
import Box from '../design-system/Box';
import Button from '../design-system/Button';
import FlexPanel from '../design-system/FlexPanel';
import Flex from '../design-system/Flex';
import Divider from '../design-system/Divider';
import Form from '../design-system/Form';
import FormField from '../design-system/FormField';
import Input from '../design-system/Input';
import {
  FacebookIcon,
  TwitterIcon,
  GoogleIcon,
} from '../design-system/SocialIcons';
import Track from '../components/Track';
import Screen from '../components/Screen';
import compose from 'lodash/fp/compose';

const Login = (login) => (
  <Screen flex={1} name="login">
    <FlexPanel bg="surface" width={[1, 1, 2 / 3, 1 / 4]} p={3}>
      <Flex alignItems="center" justifyContent="center" mb={3}>
        Login with
      </Flex>
      <Flex width={1} mt={0} p={2} justifyContent="center" alignItems="center">
        <FacebookIcon onClick={login('facebook')} />
        <GoogleIcon onClick={login('google')} />
        <TwitterIcon onClick={login('twitter')} />
      </Flex>
      <Divider width={1} text="or" />
      <Form alignItems="normal" onSubmit={login('email')}>
        <Box bg="surface" borderRadius={2} my={3} width={1}>
          <FormField boxShadow>
            <Input
              usedecoration="true"
              id="user_email"
              name="email"
              is="input"
              pr={3}
              mx={2}
              my={1}
              flex={1}
              fontSize={4}
              type="email"
              placeholder="Email"
            />
          </FormField>
          <FormField boxShadow>
            <Input
              usedecoration="true"
              id="user_pass"
              name="password"
              is="input"
              pr={3}
              mx={2}
              my={1}
              flex={1}
              fontSize={4}
              type="password"
              placeholder="Password"
            />
          </FormField>
        </Box>
        <Flex justifyContent="space-around" alignItems="center" mt={4}>
          <Track
            event="login-button"
            label="login"
            value="facebook"
            category="clicks"
          >
            <Button px={5} bg="primary" color="surface" borderColor="primary">
              Login
            </Button>
          </Track>
          <Track
            event="join-button"
            label="join"
            value="facebook"
            category="clicks"
          >
            <Button px={5} bg="primary" color="surface" borderColor="primary">
              Join
            </Button>
          </Track>
        </Flex>
      </Form>
    </FlexPanel>
  </Screen>
);

export default compose(
  withTranslation('common'),
  inject(({ app }) => ({
    login: (x) => app.login(x),
  })),
  observer
)(Login);
