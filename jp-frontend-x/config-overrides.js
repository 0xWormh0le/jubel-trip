const {
  override,
  addBabelPlugin,
  addDecoratorsLegacy,
  addWebpackAlias,
} = require('customize-cra');

const merge = require('lodash/merge');

const addJestConfig = (jestCustomConfig) => (config) => {
  config = merge(config, jestCustomConfig);
  return config;
};

const config = override(
  addBabelPlugin('emotion'),
  addDecoratorsLegacy(),
  addWebpackAlias({
    'react-emotion$': '@emotion/styled',
  })
);

config.jest = addJestConfig({
  snapshotSerializers: ['enzyme-to-json/serializer'],
  moduleNameMapper: {
    'emotion-icons': '<rootDir>/src/__mocks__/iconMocks.js',
  },
});

module.exports = config;
