import React, { useEffect, useState } from 'react';
import { inject } from 'mobx-react';

const withAccommodationOptions = (Component) => {
  const AccommodationOptionsWrapper = (props) => {
    const { loadAccommodationsOptions } = props;
    const [accommodationsOptions, setAccommodationsOptions] = useState([]);

    useEffect(() => {
      const fetchData = async () => {
        const accommodationsOptions = await loadAccommodationsOptions('');
        setAccommodationsOptions(accommodationsOptions);
      };
      fetchData();
    }, [loadAccommodationsOptions]);
    return (
      <Component {...props} accommodationsOptions={accommodationsOptions} />
    );
  };

  return inject(({ app }) => ({
    loadAccommodationsOptions: app.preferences.loadOptions('accommodations'),
  }))(AccommodationOptionsWrapper);
};

export default withAccommodationOptions;
