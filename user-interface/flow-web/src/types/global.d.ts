/// <reference types="vite/client" />

interface ImportMetaEnv {
    readonly DEV: boolean;
    readonly PROD: boolean;
    readonly MODE: string;
    readonly VITE_DEBUG?: string;
    readonly VITE_APP_NAME?: string;
    readonly VITE_API_URL?: string;
    // Add more env variables as needed
}

interface ImportMeta {
    readonly env: ImportMetaEnv;
}

declare global {
    interface Window {
        electronAPI?: {
            getAppVersion: () => Promise<string>;
            getPlatform: () => Promise<string>;
        };
        environment?: {
            isElectron: boolean;
            platform: string;
            electronVersion: string;
            nodeVersion: string;
            chromeVersion: string;
        };
    }

    interface PublicKeyCredentialCreationOptions {
        challenge: ArrayBuffer;
        rp: {
            name: string;
            id?: string;
        };
        user: {
            id: ArrayBuffer;
            name: string;
            displayName: string;
        };
        pubKeyCredParams: Array<{
            type: string;
            alg: number;
        }>;
        timeout?: number;
        excludeCredentials?: Array<{
            id: ArrayBuffer;
            type: string;
            transports?: string[];
        }>;
        authenticatorSelection?: {
            authenticatorAttachment?: string;
            requireResidentKey?: boolean;
            residentKey?: string;
            userVerification?: string;
        };
        attestation?: string;
    }
}

export { };
