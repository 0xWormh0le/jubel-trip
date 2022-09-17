import React from 'react';
import { inject, observer } from 'mobx-react';
import Screen from '../components/Screen';
import compose from 'lodash/fp/compose';

const UserProfile = ({ params }) => (
  <Screen name={`user/${match.params.id}`}>
    User id &lt;
    {match.params.id}
    &gt; profile
  </Screen>
);

export default compose(
  withTranslation('common'),
  inject(({ app, match }) => ({
    params: app.params,
  })),
  observer
)(UserProfile);
