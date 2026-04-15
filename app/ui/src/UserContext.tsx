import { createContext, useContext, useEffect, useState, type ReactNode } from 'react';
import { getMe } from './api';

interface UserContextValue {
  email: string;
  role: 'admin' | 'user';
  displayName: string;
}

const defaultUser: UserContextValue = {
  email: 'unknown',
  role: 'user',
  displayName: 'Unknown',
};

const UserContext = createContext<UserContextValue>(defaultUser);

export function UserProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<UserContextValue>(defaultUser);

  useEffect(() => {
    let cancelled = false;
    getMe()
      .then((info) => {
        if (!cancelled) {
          setUser({
            email: info.email,
            role: info.role,
            displayName: info.display_name,
          });
        }
      })
      .catch(() => {
        // Fail-safe: keep default "user" role
      });
    return () => {
      cancelled = true;
    };
  }, []);

  return <UserContext.Provider value={user}>{children}</UserContext.Provider>;
}

export function useUser(): UserContextValue {
  return useContext(UserContext);
}
