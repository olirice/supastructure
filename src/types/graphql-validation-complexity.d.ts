declare module 'graphql-validation-complexity' {
  import { ValidationRule } from 'graphql';
  
  /**
   * Creates a validation rule that limits the complexity of a GraphQL query
   * 
   * @param maxCost - The maximum complexity allowed for a query
   * @param options - Configuration options for calculating complexity
   * @returns A GraphQL validation rule
   */
  export function createComplexityLimitRule(
    maxCost: number, 
    options?: {
      scalarCost?: number;
      objectCost?: number;
      listFactor?: number;
      onCost?: (cost: number) => void;
      onField?: (field: any, args: any) => number | null;
    }
  ): ValidationRule;
} 