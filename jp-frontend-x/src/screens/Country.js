import { useEffect } from 'react';
import { inject } from 'mobx-react';

const Country = ({ match, selectCountry }) => {
  const { tripId, countryCode } = match.params;
  useEffect(() => {
    selectCountry(Number(tripId), countryCode);
  }, [tripId, countryCode, selectCountry]);
  return null;
};

export default inject(({ app }) => ({
  selectCountry: app.selectCountry,
  close: app.route.countryProfilePanelToggle.close,
}))(Country);
