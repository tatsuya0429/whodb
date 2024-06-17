import { AnimatePresence, motion } from "framer-motion";
import { FC, ReactNode } from "react";
import { twMerge } from "tailwind-merge";
import { IInternalRoute } from "../config/routes";
import { Breadcrumb } from "./breadcrumbs";
import { Sidebar } from "./sidebar/sidebar";
import { useAppSelector } from "../store/hooks";
import { Loading } from "./loading";

type IPageProps = {
    className?: string;
    children: ReactNode;
}

export const Page: FC<IPageProps> = (props) => {
    return <div className="flex grow px-8 pt-6 flex-col h-full w-full">
        <AnimatePresence>
            <motion.div className={twMerge("flex flex-row grow flex-wrap gap-2 w-full h-full overflow-y-auto", props.className)}
                initial={{ opacity: 0 }}
                animate={{ opacity: 100, }}
                exit={{ opacity: 0 }}>
                    {props.children}
            </motion.div>
        </AnimatePresence>
    </div>
}

type IInternalPageProps = IPageProps & {
    children: ReactNode;
    routes?: IInternalRoute[];
}

export const InternalPage: FC<IInternalPageProps> = (props) => {
    const schema = useAppSelector(state => state.common.schema);

    return (
        <div className="flex grow h-full w-full">
            <Sidebar />
            <Page {...props}>
                <div className="flex flex-col grow">
                    <Breadcrumb routes={props.routes ?? []} active={props.routes?.at(-1)} />
                    {
                        schema.length === 0
                        ? <Loading />
                        : <div className="flex grow flex-wrap gap-2 py-4 content-start">
                            {props.children}
                        </div>
                    }
                </div>
            </Page>
        </div>
    )
}