const identity = process.env.DELTA_MAC_SIGN_IDENTITY;
const { APPLE_ID, APPLE_APP_SPECIFIC_PASSWORD, APPLE_TEAM_ID } = process.env;
const notarization = [APPLE_ID, APPLE_APP_SPECIFIC_PASSWORD, APPLE_TEAM_ID];
if (notarization.some(Boolean) && (!identity || !notarization.every(Boolean))) {
  throw new Error("Set the signing identity and all three Apple notarization variables.");
}

module.exports = {
  packagerConfig: {
    asar: true,
    appBundleId: "io.delta.gateway.desktop",
    ignore: [/^\/test(?:\/|$)/],
    ...(identity ? { osxSign: { identity } } : {}),
    ...(notarization.every(Boolean) ? {
      osxNotarize: { appleId: APPLE_ID, appleIdPassword: APPLE_APP_SPECIFIC_PASSWORD, teamId: APPLE_TEAM_ID },
    } : {}),
  },
  makers: [
    { name: "@electron-forge/maker-dmg", platforms: ["darwin"] },
    { name: "@electron-forge/maker-zip", platforms: ["darwin"] },
  ],
};
