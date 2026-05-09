import { useMsal } from "@azure/msal-react";
import { loginRequest } from "../authConfig";

export function useApiToken() {
  const { instance, accounts } = useMsal();

  async function getToken(): Promise<string> {
    const account = accounts[0];
    try {
      const result = await instance.acquireTokenSilent({ ...loginRequest, account });
      return result.accessToken;
    } catch {
      const result = await instance.acquireTokenPopup({ ...loginRequest, account });
      return result.accessToken;
    }
  }

  return { getToken };
}
